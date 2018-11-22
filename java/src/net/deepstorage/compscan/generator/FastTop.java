package net.deepstorage.compscan.generator;

import java.util.*;

/**
 * selects top M (unsorted) out of N values using ~2*N comparisons
 */

public class FastTop<T>{
   public final List<T> result=new ArrayList<>();
   
   private final List<T> gt=new ArrayList<>();
   private final List<T> eq=new ArrayList<>();
   private final List<T> lt=new ArrayList<>();
   
   public void select(List<T> values, final int m, Comparator<T> ord){
      select(values, values.size(), m, ord);
   }
   
   public void select(List<T> values, final int size, final int m, Comparator<T> ord){
      init(gt,size);
      init(lt,size);
      init(eq,size);
      result.clear();
      select0(values, size, m, ord);
   }
   
   private void select0(List<T> values, final int size, final int m, Comparator<T> ord){
      if(m==0) return;
      if(m>=size){
         result.addAll(values);
         return;
      }
      T sample=middle(values.get(0), values.get(size/2), values.get(size-1), ord);
      int nGt=0;
      int nLt=0;
      int nEq=0;
      for(int i=size; i-->0;) {
         T v=values.get(i);
         int d=ord.compare(v,sample);
         if(d>0) gt.set(nGt++,v);
         else if(d<0) lt.set(nLt++,v);
         else eq.set(nEq++,v);
      }
      if(m<nGt){
         select0(gt, nGt, m, ord);
         return;
      }
      result.addAll(gt.subList(0,nGt));
      if(m<nGt+nEq){
         for(int i=m-nGt; i-->0;) result.add(eq.get(i));
         return;
      }
      result.addAll(eq.subList(0,nEq));
      select0(lt, nLt, m-nEq-nGt, ord);
   }
   
   private static <T> T middle(T v1, T v2, T v3, Comparator<T> ord){
      return gt(v3,v1,ord)?
         gt(v2,v3,ord)? v3: gt(v1,v2,ord)? v1: v2  :
         gt(v2,v1,ord)? v1: gt(v3,v2,ord)? v3: v2
      ;
   }
   
   private static <T> boolean gt(T v1, T v2, Comparator<T> ord){
      return ord.compare(v1,v2)>0;
   }
   
   private static void init(List<?> list, int size){
      while(list.size()<size) list.add(null);
   }
   
   public static void main(String[] args){
      Comparator<Integer> ord=(i1,i2)->Integer.compare(i1,i2);
      FastTop<Integer> top=new FastTop<>();
//      for(int i=5;i-->0;){
//         List<Integer> data=randomArray(15,100);
      for(int i=2;i-->0;){
         List<Integer> data=Arrays.asList(new Integer[]{97, 75, 70, 68, 66, 63, 59, 51, 49, 49, 24, 21, 10, 5, 2});
         top.select(data,4,ord);
         Collections.sort(top.result,ord);
         Collections.reverse(top.result);
         System.out.println(top.result);
         Collections.sort(data,ord);
         Collections.reverse(data);
         System.out.println(data);
      }
   }
   
   static List<Integer> randomArray(int size, int max){
      List<Integer> data=new ArrayList<>();
      Random r=new Random();
      for(int i=size;i-->0;) data.add(r.nextInt(max));
      return data;
   }
}
